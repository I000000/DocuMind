package gateway

import (
	"context"
	"fmt"

	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"

	documentv1 "github.com/I000000/DocuMind/gen/documind/document/v1"
)

// DocumentClient — gRPC-клиент к Document Service.
//
// Оборачивает сгенерированный клиент и добавляет:
//   - автопропагацию trace context через otelgrpc stats handler;
//   - передачу JWT-токена клиента в gRPC metadata.
//
// Используется только для CRUD-операций (Get/List/Delete).
// Upload остаётся на HTTP и проксируется через httputil.ReverseProxy.
type DocumentClient struct {
	conn   *grpc.ClientConn
	client documentv1.DocumentServiceClient
	logger *zap.Logger
}

// NewDocumentClient создаёт gRPC-клиент к document-service.
// Соединение устанавливается лениво — NewClient не блокируется.
func NewDocumentClient(addr string, logger *zap.Logger) (*DocumentClient, error) {
	conn, err := grpc.NewClient(addr,
		// В dev используем plaintext. В prod — TLS или mTLS.
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		// otelgrpc stats handler прокидывает traceparent в metadata
		// и создаёт client-span на каждый RPC.
		grpc.WithStatsHandler(otelgrpc.NewClientHandler()),
	)
	if err != nil {
		return nil, fmt.Errorf("dial document-service: %w", err)
	}

	return &DocumentClient{
		conn:   conn,
		client: documentv1.NewDocumentServiceClient(conn),
		logger: logger.With(zap.String("component", "document_client")),
	}, nil
}

func (c *DocumentClient) Close() error {
	return c.conn.Close()
}

// GetDocument возвращает документ по ID.
func (c *DocumentClient) GetDocument(
	ctx context.Context,
	token, id string,
) (*documentv1.Document, error) {
	ctx = withAuthToken(ctx, token)
	resp, err := c.client.GetDocument(ctx, &documentv1.GetDocumentRequest{Id: id})
	if err != nil {
		return nil, err
	}
	return resp.Document, nil
}

// ListDocuments возвращает страницу документов и общее количество.
func (c *DocumentClient) ListDocuments(
	ctx context.Context,
	token string,
	pageSize int,
) ([]*documentv1.Document, int32, error) {
	ctx = withAuthToken(ctx, token)
	resp, err := c.client.ListDocuments(ctx, &documentv1.ListDocumentsRequest{
		PageSize: int32(pageSize),
	})
	if err != nil {
		return nil, 0, err
	}
	return resp.Documents, resp.TotalCount, nil
}

// DeleteDocument удаляет документ и все его чанки.
func (c *DocumentClient) DeleteDocument(
	ctx context.Context,
	token, id string,
) error {
	ctx = withAuthToken(ctx, token)
	_, err := c.client.DeleteDocument(ctx, &documentv1.DeleteDocumentRequest{Id: id})
	return err
}

// withAuthToken кладёт Bearer-токен в outgoing gRPC metadata.
// document-service читает его через AuthInterceptor.
func withAuthToken(ctx context.Context, token string) context.Context {
	if token == "" {
		return ctx
	}
	return metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+token)
}

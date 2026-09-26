package auth

// User — аутентифицированный пользователь из JWT.
type User struct {
	Subject           string   // sub — уникальный ID в Keycloak
	Email             string   // email
	PreferredUsername string   // preferred_username
	Roles             []string // realm_access.roles
}

// HasRole проверяет наличие роли.
func (u *User) HasRole(role string) bool {
	for _, r := range u.Roles {
		if r == role {
			return true
		}
	}
	return false
}

// HasAnyRole проверяет наличие хотя бы одной из ролей.
func (u *User) HasAnyRole(roles ...string) bool {
	for _, r := range roles {
		if u.HasRole(r) {
			return true
		}
	}
	return false
}

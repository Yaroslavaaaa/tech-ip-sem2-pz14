package service

type AuthService struct {
	validTokens map[string]string
}

func NewAuthService() *AuthService {
	return &AuthService{
		validTokens: map[string]string{
			"demo-token": "student",
		},
	}
}

func (s *AuthService) Login(username, password string) (string, bool) {
	if username == "student" && password == "student" {
		return "demo-token", true
	}
	return "", false
}

func (s *AuthService) Verify(token string) (string, bool) {
	username, exists := s.validTokens[token]
	return username, exists
}

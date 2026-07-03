# Surbi — Docker Compose 실행 가이드

## 사전 요구사항
- Docker 24+
- Docker Compose v2.20+
- WSL2 내부 경로에서 실행 권장 (Windows 환경)

---

## 로컬 개발 환경

```bash
# 1. 환경변수 설정
cp .env.example .env
# .env 파일을 열어 각 값 입력

# 2. 최초 실행 (빌드 포함)
docker compose -f docker-compose.yml -f docker-compose.local.yml up -d --build

# 3. 로그 확인
docker compose -f docker-compose.yml -f docker-compose.local.yml logs -f api

# 4. 중지
docker compose -f docker-compose.yml -f docker-compose.local.yml down
```

### 접근 주소 (로컬)
| 서비스 | 주소 |
|--------|------|
| FastAPI (직접) | http://localhost:8000 |
| FastAPI Docs | http://localhost:8000/docs |
| Nginx 경유 | http://localhost/api/ |
| PostgreSQL | localhost:5432 |
| Redis | localhost:6379 |

### 편의 별칭 등록 (.bashrc / .zshrc)
```bash
alias dc-local="docker compose -f docker-compose.yml -f docker-compose.local.yml"
alias dc-prod="docker compose -f docker-compose.yml -f docker-compose.prod.yml"
```

---

## 프로덕션 환경

```bash
# 1. 환경변수 설정 (ENVIRONMENT=production으로 변경)
cp .env.example .env

# 2. Cloudflare Origin 인증서 배치
sudo mkdir -p /etc/ssl/cloudflare
sudo cp origin.crt /etc/ssl/cloudflare/origin.crt
sudo cp origin.key /etc/ssl/cloudflare/origin.key

# 3. Flutter Web 빌드 파일 배치
sudo mkdir -p /var/www/surbi/web
sudo cp -r build/web/* /var/www/surbi/web/

# 4. 배포
docker compose -f docker-compose.yml -f docker-compose.prod.yml up -d --build
```

---

## 브랜치 전략
| 브랜치 | 용도 |
|--------|------|
| main | 프로덕션 배포 기준 |
| dev | 통합 개발 브랜치 |
| feature/* | 기능 개발 |
| hotfix/* | 긴급 수정 |

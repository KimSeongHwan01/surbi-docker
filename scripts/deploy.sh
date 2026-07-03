#!/bin/bash
# scripts/deploy.sh
# 프로덕션 무중단 배포 스크립트 (CI/CD에서 호출)
set -e

echo "▶ [Surbi] 배포 시작: $(date)"

# 최신 이미지 빌드
docker compose -f docker-compose.yml -f docker-compose.prod.yml build --no-cache api worker

# api, worker만 재시작 (postgres, redis, nginx 무중단 유지)
docker compose -f docker-compose.yml -f docker-compose.prod.yml up -d --no-deps api worker

# 헬스체크 대기 (최대 30초)
echo "▶ 헬스체크 대기 중..."
for i in $(seq 1 6); do
  STATUS=$(curl -s -o /dev/null -w "%{http_code}" http://localhost:8000/health || echo "000")
  if [ "$STATUS" = "200" ]; then
    echo "✅ 배포 성공 (HTTP $STATUS)"
    exit 0
  fi
  echo "  대기 중... ($i/6)"
  sleep 5
done

echo "❌ 헬스체크 실패 — 롤백 필요"
exit 1

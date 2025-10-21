docker build  -t edk:latest -f Dockerfile.release .
VERSION=$(poetry version -s)

REPO_URL="ghcr.io/earth-data-kit/edk"

cat .token | docker login ghcr.io -u siddhantgupta3 --password-stdin

echo "$REPO_URL:latest"
docker tag edk:latest "$REPO_URL:latest"
docker tag edk:latest "$REPO_URL:v$VERSION"

docker push "$REPO_URL:v$VERSION"
docker push "$REPO_URL:latest"

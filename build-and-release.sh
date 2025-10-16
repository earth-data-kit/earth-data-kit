docker build  -t edk:latest -f Dockerfile.release .
VERSION=$(poetry version -s)

REPO_URL = "ghcr.io/earth-data-kit/edk"
docker tag edk:latest "${REPO_URL}:latest"
docker tag edk:latest "${REPO_URL}:${VERSION}"

docker push "${REPO_URL}:${VERSION}"
docker push "${REPO_URL}:latest"

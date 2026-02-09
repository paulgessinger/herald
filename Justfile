run:
    dotenvx run -- uv run quart -A herald.web run

test:
    uv run pytest

image_url := "ghcr.io/acts-project/ci-bridge"
sha := "sha-" + `git rev-parse --short HEAD`
image:
    docker build --platform linux/amd64 -t {{image_url}}:{{sha}} .
    docker tag {{image_url}}:{{sha}} {{image_url}}:latest
    docker push {{image_url}}:{{sha}}
    docker push {{image_url}}:latest


deploy: image
    sleep 1
    oc import-image ci-bridge --all

docker:
    docker run --rm -it -e HERALD_GH_PRIVATE_KEY=blub -e HERALD_GH_APP_ID=123 -e HERALD_METRICS_SECRET=123 -v$PWD/files:/app/cache/files -v$PWD/artifacts:/app/cache/artifacts test

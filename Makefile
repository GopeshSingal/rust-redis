IMAGE_NAME = rust-redis-dev

dev-build:
	docker build -t $(IMAGE_NAME) -f Dockerfile.dev .

dev-shell:
	docker run -it --rm \
		-p 6379:6379 \
		-v "$(PWD)":/workspace \
		-w /workspace \
		$(IMAGE_NAME) \
		bash

dev-run:
	docker run -it --rm \
		-p 6379:6379 \
		-v "$(PWD)":/workspace \
		-w /workspace \
		$(IMAGE_NAME) \
		cargo run

dev-test:	
	docker run -it --rm \
		-p 6379:6379 \
		-v "$(PWD)":/workspace \
		-w /workspace \
		$(IMAGE_NAME) \
		cargo test

.DEFAULT_GOAL := help
BIN_DIR := bin
GO ?= go

.PHONY: help
help: ## 显示可用目标
	@grep -hE '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-16s\033[0m %s\n", $$1, $$2}'

.PHONY: build
build: ## 编译 notifyd 和 mockvendor 到 bin/
	$(GO) build -o $(BIN_DIR)/notifyd ./cmd/notifyd
	$(GO) build -o $(BIN_DIR)/mockvendor ./cmd/mockvendor

.PHONY: test
test: ## 运行全部测试
	$(GO) test -count=1 ./...

.PHONY: test-race
test-race: ## 带竞态检测运行全部测试
	$(GO) test -race -count=1 ./...

.PHONY: cover
cover: ## 生成覆盖率报告（coverage.html）
	$(GO) test -count=1 -coverprofile=coverage.out -coverpkg=./internal/... ./...
	$(GO) tool cover -func=coverage.out | tail -1
	$(GO) tool cover -html=coverage.out -o coverage.html
	@echo "报告已生成：coverage.html"

.PHONY: vet
vet: ## 静态检查
	$(GO) vet ./...

.PHONY: fmt
fmt: ## 格式化代码
	gofmt -w .

.PHONY: fmt-check
fmt-check: ## 校验格式（CI 用）
	@out="$$(gofmt -l .)"; \
	if [ -n "$$out" ]; then echo "以下文件未格式化："; echo "$$out"; exit 1; fi

.PHONY: check
check: fmt-check vet test ## 提交前的完整校验

.PHONY: validate
validate: ## 校验配置文件
	$(GO) run ./cmd/notifyd -config config.yaml -validate

.PHONY: run
run: ## 用 config.yaml 启动服务（需要先起 mockvendor，或改成真实地址）
	$(GO) run ./cmd/notifyd -config config.yaml -log-level debug

.PHONY: demo
demo: ## 一键演示：故障 → 重试 → 恢复 → 死信 → 重投 → 熔断 → 隔离
	./scripts/demo.sh

.PHONY: clean
clean: ## 清理产物与本地数据
	rm -rf $(BIN_DIR) data coverage.out coverage.html

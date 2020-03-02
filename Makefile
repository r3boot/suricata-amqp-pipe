TARGET = suricata-amqp-pipe
BUILD_DIR = ./build

all: $(TARGET)

$(BUILD_DIR):
	mkdir -p "$(BUILD_DIR)"

$(TARGET): $(BUILD_DIR)
	go build -v -o "$(BUILD_DIR)/$(TARGET)" "cmd/$(TARGET)/main.go"

clean:
	[[ -d "$(BUILD_DIR)" ]] && rm -rf "$(BUILD_DIR)" || true

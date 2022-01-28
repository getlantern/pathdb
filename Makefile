# requires github.com/pseudomuto/protoc-gen-doc plugin
pbuf_test.pb.go: pbuf_test.proto
	@protoc --go_out=$$GOPATH/src pbuf_test.proto && \
	echo "recompiled protocol buffers"
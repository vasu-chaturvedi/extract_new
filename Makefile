build:
	go build -mod=vendor
run:
	./extract_new -appCfg=./config/config.json -runCfg=./config/extraction/RetailCif.json -mode="E"

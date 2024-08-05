build-shared-libs:
	cd spacetime_tools/stitching/shared_libs/spacetime_tools; go build -o ../builds/go-lib ./main.go

run-tests:
	python3 -m pytest . -rx
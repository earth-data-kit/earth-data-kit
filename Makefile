build-shared-libs:
	cd earth_data_kit/stitching/shared_libs/earth_data_kit; go build -o ../builds/go-lib ./main.go

run-tests:
	python3 -m pytest . -rx

build-package:
	poetry build

install-package:
	pip3 install dist/earth_data_kit-1.0.0a1.tar.gz

build-docs:
	rm -rf docs/build/*
	sphinx-build -M html docs/source/ docs/build/

rebuild-docs:
	make build-package
	make install-package
	make build-docs
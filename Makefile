build-shared-libs:
	cd spacetime_tools/stitching/shared_libs/spacetime_tools; go build -o ../builds/go-lib ./main.go

run-tests:
	python3 -m pytest . -rx

build-package:
	python3 setup.py sdist bdist_wheel

build-docs:
	rm -rf docs/build/*
	sphinx-build -M html docs/source/ docs/build/
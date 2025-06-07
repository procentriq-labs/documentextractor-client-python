# Build the distribution packages
build:
    python -m build

# Upload to PyPI
publish: build
    twine upload dist/*

# Clean build artifacts
clean:
    rm -rf build dist *.egg-info

# Check if the package will render properly on PyPI
check-readme:
    twine check dist/*
all:
	@echo "Nothing to make"
	@echo "Run \"sudo make install\" to install"
	@echo "(equivalent to \"sudo python setup.py install\")"

install:
	sudo python setup.py install

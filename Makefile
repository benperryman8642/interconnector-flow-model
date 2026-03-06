.PHONY: \
	clean-bronze-elexon-fuelhh \
	clean-bronze-elexon \
	clean-bronze \
	clean-silver-elexon \
	clean-silver \
	clean-manifests \
	clean

clean-bronze-elexon-fuelhh:
	rm -rf data/bronze/elexon/fuelhh

clean-bronze-elexon:
	rm -rf data/bronze/elexon

clean-manifests:
	find data/bronze -type f -name "*ingest_log.parquet" -delete

clean-bronze:
	@echo "This will delete all files under data/bronze"
	@read -p "Continue? [y/N] " ans; \
	if [ "$$ans" = "y" ] || [ "$$ans" = "Y" ]; then \
		rm -rf data/bronze/*; \
	else \
		echo "Cancelled"; \
	fi

clean-silver-elexon:
	rm -rf data/silver/elexon

clean-silver:
	@echo "This will delete all files under data/silver"
	@read -p "Continue? [y/N] " ans; \
	if [ "$$ans" = "y" ] || [ "$$ans" = "Y" ]; then \
		rm -rf data/silver/*; \
	else \
		echo "Cancelled"; \
	fi

clean:
	@echo "This will delete all files under data/bronze and data/silver"
	@read -p "Continue? [y/N] " ans; \
	if [ "$$ans" = "y" ] || [ "$$ans" = "Y" ]; then \
		rm -rf data/bronze/* data/silver/*; \
	else \
		echo "Cancelled"; \
	fi
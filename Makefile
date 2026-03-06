.PHONY: clean-bronze-elexon-fuelhh clean-bronze-elexon clean-bronze clean-manifests

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
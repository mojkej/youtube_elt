PROJECT := youtube_elt
CONDA := conda
ENV_FILE := environment.yml

# Met à jour l'environnement conda à partir du fichier environment.yml
conda-env-update:
	$(CONDA) env update -f $(ENV_FILE)
	@echo "✅ Environnement conda mis à jour!"

# Formater le code selon PEP8
format:
	autopep8 --in-place --aggressive --recursive .
	isort .
	@echo "✅ Formatage PEP8 terminé!"

type-check:
	python -m mypy .
	@echo "✅ Vérification de type terminée!"
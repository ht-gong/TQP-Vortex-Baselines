# The one entry point. Every target runs in the pinned container through
# docker/run.sh; the scripts it calls keep their logic, Make only forwards
# variables. Box-specific settings (DATA_DIR, SCRATCH, SHM, GPU) come from
# local.env (copy local.env.example).
#
#   make image                                 build the image for the current pins
#   make doctor                                versions, GPU, io_uring, free space
#   make data SF=100 / make validate SF=100    generate (if missing) + validate $DATA_DIR/sf100
#   make pth SF=1                              export $DATA_DIR/sf1 as TQP-Vortex .pth files
#   make bench SF=100 [ENGINES="..."]          TPC-H q1-22 -> results/all_results.csv
#   make smoke                                 SF1, all engines, nothing merged
#   make ablation SF=100 [ENCODINGS= CODECS= ROUNDS= ENGINES= OUT=]
#                                              format profiling pass -> per-column format map
#   make format-map SF=100 [TOL=0.05 OUT=]     re-run only the map selection
#   make summary                               pivots of results/all_results.csv
#   make shell                                 a shell in the container

RUN := docker/run.sh
ENCODINGS ?= plain dict delta
CODECS ?= snappy zstd lz4raw
ROUNDS ?= 3
TOL ?= 0.05
OUT ?= results

need = $(if $($(1)),,$(error $(1) is required, e.g. `make $@ $(1)=$(2)`))

.PHONY: image shell doctor data validate pth bench smoke ablation format-map summary

image:
	$(RUN) build

shell:
	$(RUN) bash

doctor:
	$(RUN) docker/doctor.sh

data:
	$(call need,SF,100)
	$(RUN) datagen/dataset.sh $(SF)

validate:
	$(call need,SF,100)
	$(RUN) bash -c '"$$PY" results/validate_dataset.py "$$DATA_DIR/sf$(SF)/parquet" $(SF)'

pth:
	$(call need,SF,1)
	$(RUN) bash -c '"$$PY" datagen/export_pth.py "$$DATA_DIR/sf$(SF)/parquet" $(SF) "$$DATA_DIR/pth"'

bench:
	$(call need,SF,100)
	$(RUN) ./run.sh $(SF)

smoke:
	$(RUN) bash -c 'RUN_CSV_DIR="$$SCRATCH/smoke" ./run.sh 1'

ablation:
	$(call need,SF,100)
	$(RUN) bash -c '"$$PY" ablation/format_profile.py run --sf $(SF) --encodings "$(ENCODINGS)" \
	  --codecs "$(CODECS)" --rounds $(ROUNDS) --tol $(TOL) --out "$(OUT)" \
	  $${ENGINES:+--engines "$$ENGINES"}'

format-map:
	$(call need,SF,100)
	$(RUN) bash -c '"$$PY" ablation/format_profile.py select --sf $(SF) --tol $(TOL) --out "$(OUT)"'

summary:
	$(RUN) bash -c '"$$PY" results/summary.py'

export PYTHONPATH="${PYTHONPATH}:/cerebralcortex/code/ali/CerebralCortex/"

CC_CONFIG="/cerebralcortex/code/ali/cc_config/cc_configuration.yml"

python3.6 comp.py -conf $CC_CONFIG -start $1 -end $2 -participants $3
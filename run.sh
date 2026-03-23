# First, build the list of -C arguments
FLAGS=$(for jar in /home/sathviksrao/IIITB/Sem8/PE/Watermark/dependencies/*.jar; do echo -n "-C file://$jar "; done)

# Then, run Flink with those flags
/opt/flink/bin/flink run -c StreamingJob $FLAGS span.jar config.properties
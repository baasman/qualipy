.PHONY: docker_build_run
id = $(shell sudo docker ps -q -f "ancestor=qualipy")

docker_build_run:
	sudo docker container stop $(id)
	sudo docker build -t qualipy .
	sudo docker run -p 5008:5008 --rm --name qualipy_prod \
	    --mount type=bind,source=/data/baasman/.qualipy/,target=/root/.qualipy \
	    qualipy qualipy run --port 5008 --config_dir /root/.qualipy

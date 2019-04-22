## Qualipy

to run webapp:

sudo docker build -t qualipy:latest .

sudo docker run -it -p 5006:5005 --mount type=bind,source=/home/baasman/.qualipy-prod/,target=/root/.qualipy qualipy:latest /bin/bash
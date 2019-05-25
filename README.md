## Qualipy


## Install

### from Git
```bash
git clone https://github.com/baasman/qualipy.git
pip install .
```
### from pip
```bash
pip install qualipy
```

## Running the webapp
```bash
cd web/

sudo docker build -t qualipy:latest .

sudo docker run -it -p 5006:5005 --mount type=bind,source=/home/<username>/.qualipy-prod/,target=/root/.qualipy qualipy:latest /bin/bash
```


## How to use

###

to run webapp:

sudo docker build -t qualipy:latest .

sudo docker run -it -p 5006:5005 --mount type=bind,source=/home/baasman/.qualipy-prod/,target=/root/.qualipy qualipy:latest /bin/bash
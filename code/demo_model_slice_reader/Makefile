
IMAGE=public-images-registry.cn-hangzhou.cr.aliyuncs.com/public/alimama-2023:v0

all: 
	g++ demo_model_slice_reader.cpp -lmodel_slice_reader -o demo_model_slice_reader
bash:
	docker run --rm -it  -v ./:/work  -w /work --network alimama $(IMAGE) bash

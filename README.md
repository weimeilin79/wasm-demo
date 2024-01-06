
# Maybe it's better to do a proper docker compose file



# Download latest stable image
docker pull tensorflow/tensorflow:latest  

# Start Jupyter server 
docker run -it -p 8888:8888 tensorflow/tensorflow:latest-jupyter  
docker network connect redpanda-quickstart_redpanda_network 86fecf59ce0b



# Reference
https://thecleverprogrammer.com/2023/01/02/food-delivery-time-prediction-using-python/
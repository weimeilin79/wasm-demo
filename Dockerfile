# Use the official TensorFlow image as a parent image
FROM tensorflow/tensorflow:latest-jupyter

# Set the working directory in the container
WORKDIR /usr/src/app

# Copy the project files into the container at /usr/src/app
COPY ./delivery-model /usr/src/app

# (Optional) Install any dependencies
RUN pip install -r requirements.txt

# Make port 8888 available to the world outside this container
EXPOSE 8888

# Run Jupyter Notebook when the container launches
CMD ["jupyter", "notebook", "--ip=0.0.0.0", "--port=8888", "--allow-root", "--NotebookApp.token='redpanda'", "--NotebookApp.password='redpanda'"]

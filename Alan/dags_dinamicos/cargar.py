from airflow.hooks.S3_hook import S3Hook

def cargar(bucket_name, file, key):

    #Se utiliza la conexion previamente definida en airflow
    hook = S3Hook('my_conn_s3')
    
    hook.load_file(
        bucket_name= bucket_name,
        key= key, filename= file,
        replace= True
        )
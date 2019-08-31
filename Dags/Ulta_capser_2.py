from __future__ import print_function

from datetime import datetime, timedelta

from airflow import models, DAG
from airflow.operators import bash_operator
from airflow.operators import python_operator
from airflow.contrib.kubernetes import pod
from airflow.contrib.kubernetes import secret
from airflow.models import Variable
from airflow.contrib.kubernetes.volume import Volume
from airflow.contrib.kubernetes.volume_mount import VolumeMount
from airflow.contrib.operators import kubernetes_pod_operator




# secret_file = secret.Secret(
#     # Expose the secret as environment variable.
#     deploy_type='volume',
#     # The name of the environment variable, since deploy_type is `env` rather
#     # than `volume`.
#     deploy_target='/var/secrets/google',
#     # Name of the Kubernetes Secret
#     secret='casper-worker-key',
#     # Key of a secret stored in this Secret object
#     key='google-cloud-key')


affinity_values={
        'nodeAffinity': {
            # requiredDuringSchedulingIgnoredDuringExecution means in order
            # for a pod to be scheduled on a node, the node must have the
            # specified labels. However, if labels on a node change at
            # runtime such that the affinity rules on a pod are no longer
            # met, the pod will still continue to run on the node.
            'requiredDuringSchedulingIgnoredDuringExecution': {
                'nodeSelectorTerms': [{
                    'matchExpressions': [{
                        # When nodepools are created in Google Kubernetes
                        # Engine, the nodes inside of that nodepool are
                        # automatically assigned the label
                        # 'cloud.google.com/gke-nodepool' with the value of
                        # the nodepool's name.
                        'key': 'cloud.google.com/gke-nodepool',
                        'operator': 'In',
                        # The label key's value that pods can be scheduled
                        # on.
                        'values': [
                            'pool-1',
                        ]
                    }]
                }]
            }
        }
    }

volume_mount_code = VolumeMount('my-volume',
                            mount_path='/home/git/',
                            sub_path=None,
                            read_only=True)

volume_mount_key = VolumeMount('google-cloud-key',
                            mount_path='/var/secrets/google',
                            sub_path=None,
                            read_only=False)
volume_config_key= {
    'secret':
      {
        'secretName': 'casper-worker-key'
      }
    }

volume_config= {
    'persistentVolumeClaim':
      {
        'claimName': 'my-vol'
      }
    }

volume_code = Volume(name='my-volume', configs=volume_config)
volume_key = Volume(name='google-cloud-key', configs=volume_config_key)


default_dag_args = {
    'start_date': datetime(2018, 1, 1),
    'depends_on_past': False,
    'owner': 'Airflow',
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
    # The start_date describes when a DAG is valid / can be run. Set this to a
    # fixed point in time rather than dynamically, since it is evaluated every
    # time a DAG is parsed. See:
    # https://airflow.apache.org/faq.html#what-s-the-deal-with-start-date

}

# Define a DAG (directed acyclic graph) of tasks.
# Any task you create within the context manager is automatically added to the
# DAG object.
with DAG(
        'Ulta_Casper_2',
        catchup=False,
        schedule_interval='0 */1 * * *',
        concurrency=50,
        max_active_runs=1,
        default_args=default_dag_args) as dag:

    pod_res = pod.Resources(request_memory='100Mi',request_cpu='0.05',limit_memory='120Mi',limit_cpu='0.09')

    goodbye_bash = bash_operator.BashOperator(
        task_id='bye',
        bash_command='echo Goodbye.')

    kubernetes_min_crawl = [kubernetes_pod_operator.KubernetesPodOperator(
        # The ID specified for the task.
        task_id='ulta-crawl-2'+str(i),
        # Name of task you want to run, used to generate Pod ID.
        name='ulta-crawl-2'+str(i),
        # resources=pod_res,
        # Entrypoint of the container, if not specified the Docker container's
        # entrypoint is used. The cmds parameter is templated.
        cmds=["scrapy", "runspider","/home/git/app/crawl.py"],
        resources=pod_res,
        volumes=[volume_code,volume_key],
        env_vars={"GOOGLE_APPLICATION_CREDENTIALS":"/var/secrets/google/key.json"},
        # List of VolumeMount objects to pass to the Pod.
        volume_mounts=[volume_mount_code,volume_mount_key],
        # The namespace to run within Kubernetes, default namespace is
        # `default`. There is the potential for the resource starvation of
        # Airflow workers and scheduler within the Cloud Composer environment,
        # the recommended solution is to increase the amount of nodes in order
        # to satisfy the computing requirements. Alternatively, launching pods
        # into a custom namespace will stop fighting over resources.
        namespace='airflow-worker-pods',
        affinity=affinity_values,
        is_delete_operator_pod=True,
        config_file='/home/airflow/composer_kube_config',
        # Docker image specified. Defaults to hub.docker.com, but any fully
        # qualified URLs will point to a custom repository. Supports private
        # gcr.io images if the Composer Environment is under the same
        # project-id as the gcr.io images.
        image='docker.io/hemanthk0208/spider:v1') for i in range(50)]


    pod_res_1 = pod.Resources(request_memory='150Mi',request_cpu='0.1',limit_memory='180Mi',limit_cpu='0.12')
    kubernetes_min_spider = [kubernetes_pod_operator.KubernetesPodOperator(
        # The ID specified for the task.
        task_id='ulta-spider-2'+str(i),
        # Name of task you want to run, used to generate Pod ID.
        name='ulta-spider-2'+str(i),
        # resources=pod_res,
        # Entrypoint of the container, if not specified the Docker container's
        # entrypoint is used. The cmds parameter is templated.
        cmds=["scrapy", "runspider","/home/git/app/spider.py"],
        resources=pod_res_1,
        volumes=[volume_code,volume_key],
        env_vars={"GOOGLE_APPLICATION_CREDENTIALS":"/var/secrets/google/key.json"},
        # List of VolumeMount objects to pass to the Pod.
        volume_mounts=[volume_mount_code,volume_mount_key],
        # The namespace to run within Kubernetes, default namespace is
        # `default`. There is the potential for the resource starvation of
        # Airflow workers and scheduler within the Cloud Composer environment,
        # the recommended solution is to increase the amount of nodes in order
        # to satisfy the computing requirements. Alternatively, launching pods
        # into a custom namespace will stop fighting over resources.
        namespace='airflow-worker-pods',
        affinity=affinity_values,
        is_delete_operator_pod=True,
        config_file='/home/airflow/composer_kube_config',
        # Docker image specified. Defaults to hub.docker.com, but any fully
        # qualified URLs will point to a custom repository. Supports private
        # gcr.io images if the Composer Environment is under the same
        # project-id as the gcr.io images.
        image='docker.io/hemanthk0208/spider:v1') for i in range(50)]

    kubernetes_min_crawl >> goodbye_bash >> kubernetes_min_spider

from airflow.contrib.operators import KubernetesOperator
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.contrib.kubernetes.secret import Secret



k = KubernetesPodOperator(namespace='default',
                          image="ubuntu:16.04",
                          cmds=["bash", "-cx"],
                          arguments=["echo", "10"],
                          labels={"foo": "bar"},
                          name="test",
                          task_id="task",
                          is_delete_operator_pod=True,
                          hostnetwork=False,
                          get_logs=True
                          )


k = KubernetesPodOperator(namespace='default',
                          image="ubuntu:16.04",
                          cmds=["bash", "-cx"],
                          arguments=["echo", "10"],
                          labels={"foo": "bar"},
                          secrets=[secret_file, secret_env, secret_all_keys],
                          volumes=[volume],
                          volume_mounts=[volume_mount]
                          name="test",
                          task_id="task",
                          affinity=affinity,
                          is_delete_operator_pod=True,
                          hostnetwork=False,
                          tolerations=tolerations,
                          configmaps=configmaps,
                          get_logs=True
                          )

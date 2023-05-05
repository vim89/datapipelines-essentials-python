from datetime import datetime

from com.vitthalmirji.utils.helpers import get_user

USER = get_user()
JOB_START_TIME = datetime.now().strftime('%Y-%m-%dT%H-%M-%S-%f')
SPARK_APPLICATION_NAME = f"Spark application launched by {USER}"

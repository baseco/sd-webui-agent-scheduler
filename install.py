import launch

if not launch.is_installed("sqlalchemy"):
    launch.run_pip("install sqlalchemy", "requirement for task-scheduler")

if not launch.is_installed("psycopg2-binary"):
    launch.run_pip("install psycopg2-binary", "requirement for task-scheduler")

if not launch.is_installed("pika"):
    launch.run_pip("install pika", "requirement for task-scheduler")

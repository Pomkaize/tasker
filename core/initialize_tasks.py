import runner
import db


async def initialize_tasks():
    db.drop_tables()
    db.create_tables()

    task_ids = runner.get_tasks_ids()
    db.insert_tasks_ids(task_ids)

    print(f'Задач добавлено: {len(task_ids)}')


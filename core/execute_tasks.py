import runner
import db
import asyncio


async def execute_task_wrapper(task_id):
    print(f'Executing task with id: {task_id}')

    result = await runner.execute_task(task_id)

    return task_id, result


async def execute_tasks():
    executed_tasks_count = 0

    while True:
        tasks_ids = db.get_unprocessed_tasks_ids(runner.MAX_CONCURRENT_TASKS_COUNT)
        executed_tasks_count += len(tasks_ids)

        if len(tasks_ids) == 0:
            break

        tasks = list(map(execute_task_wrapper, tasks_ids))
        db.insert_tasks_results(await asyncio.gather(*tasks))

    print(f'Обработка задач закончена: {executed_tasks_count}')

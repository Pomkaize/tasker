import asyncio
import core


if __name__ == '__main__':
    print('Режим работы:')
    print('1 - добавить задачи для обработки')
    print('2 - продолжить обработку задач')

    variant = input()

    variants = {
        '1': core.initialize_tasks,
        '2': core.execute_tasks
    }

    variant_fn = variants.get(variant)

    if variant_fn is None:
        print('Некорректный ввод')
    else:
        asyncio.run(variant_fn())


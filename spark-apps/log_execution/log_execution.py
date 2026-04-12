def log_execution(func):
    def wrapper(*args, **kwargs):
        print(f"Загрузка {func.__name__}...")
        result = func(*args, **kwargs)
        print(f"Загрузка {func.__name__} успешно завершена")
        return result

    return wrapper

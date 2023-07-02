import random

from prefect import flow, task


@task(name="Add two numbers", retry_delay_seconds=2, retries=3)
def add(x: int, y: int) -> int:
    # Simulate a random error
    if random.random() < 0.4:
        raise ValueError("Something went wrong")
    return x + y

@flow(name="Main flow")
def main_flow(number_a: int=1, number_b: int=1) -> int:
    return add(number_a, number_b)

if __name__ == "__main__":
    main_flow(number_a=1, number_b=2)
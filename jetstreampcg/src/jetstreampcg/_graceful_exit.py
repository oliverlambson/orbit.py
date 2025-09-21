"""Minimal example of graceful exit implementation for reference in development of the consumers"""

import asyncio
import logging
import random


async def watch_done(future: asyncio.Future[None]) -> None:
    """A task that will complete when the done future result is set"""
    logger = logging.getLogger("watch_done")

    try:
        logger.info("awaiting future...")
        await future
        logger.info("future set")
    except asyncio.CancelledError:
        logger.info("graceful shutdown complete")
        raise


async def watch_cancel(event: asyncio.Event) -> None:
    """A task that will complete when the cancel event is set"""
    logger = logging.getLogger("watch_cancel")

    try:
        logger.info("awaiting event...")
        _ = await event.wait()
        logger.info("event set")
    except asyncio.CancelledError:
        logger.info("graceful shutdown complete")
        raise


async def main_loop() -> None:
    """Where the real work actually happens"""
    logger = logging.getLogger("main_loop")

    try:
        while True:
            logger.info("doing work")
            _ = await asyncio.sleep(1)
    except asyncio.CancelledError:
        logger.info("received cancellation signal...")
        await asyncio.sleep(random.uniform(2, 4))  # noqa: S311
        logger.info("graceful shutdown complete")
        raise


async def supervisor(done: asyncio.Future[None], cancel: asyncio.Event) -> None:
    """Randomly sets done or cancel"""
    logger = logging.getLogger("supervisor")

    try:
        while True:
            logger.info("supervising")
            _ = await asyncio.sleep(1)
            x = random.random()  # noqa: S311
            if x < 0.8:
                logger.info("continue")
            elif 0.8 <= x < 0.9:
                if not done.done():
                    done.set_result(None)
                    logger.info("set done result")
            elif 0.9 <= x < 1.0:
                if not cancel.is_set():
                    cancel.set()
                    logger.info("set cancel")

    except asyncio.CancelledError:
        logger.info("received cancellation signal...")
        await asyncio.sleep(1)
        logger.info("graceful shutdown complete")
        raise


async def main() -> None:
    """Creates all concurrent tasks"""
    logger = logging.getLogger("main")

    done_future = asyncio.Future[None]()
    cancel_event = asyncio.Event()

    tasks = [
        asyncio.create_task(watch_done(done_future), name="watch_done"),
        asyncio.create_task(watch_cancel(cancel_event), name="watch_cancel"),
        asyncio.create_task(supervisor(done_future, cancel_event), name="supervisor"),
        asyncio.create_task(main_loop(), name="main_loop"),
    ]
    logger.info("tasks created")
    try:
        logger.info("waiting for first task to complete")
        _, pending = await asyncio.wait(
            tasks,
            return_when=asyncio.FIRST_COMPLETED,
        )

        if pending:
            try:
                _ = await asyncio.wait_for(asyncio.gather(*pending), timeout=3)
            except asyncio.TimeoutError:
                logger.warning("graceful shutdown timeout - forcing exit")

    except asyncio.CancelledError:
        logger.info("graceful shutdown requested, waiting...")
        _, pending = await asyncio.wait(tasks, timeout=3)
        if pending:
            for p in pending:
                _ = p.cancel()
                logger.warning("%s didn't complete graceful shutdown", p.get_name())


def entrypoint() -> None:
    """Program entrypoint"""
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s::%(message)s"
    )
    logger = logging.getLogger("entrypoint")

    logger.info("launching main")
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("goodbye.")


if __name__ == "__main__":
    entrypoint()

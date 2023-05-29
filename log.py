from loguru import logger


def main():
    logger.trace("This is a trace message.")
    logger.debug("This is a debug message")
    logger.info("This is an info message.")
    logger.success("This is a success message.")
    logger.warning("This is a warning message.")
    logger.error("This is an error message.")
    logger.critical("This is a critical message.")


if __name__ == "__main__":
    main()

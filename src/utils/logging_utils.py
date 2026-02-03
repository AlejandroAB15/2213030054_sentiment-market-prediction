def log_and_print(logger, message, level="info"):
    print(message)

    if level == "info":
        logger.info(message)
    elif level == "warning":
        logger.warning(message)
    elif level == "error":
        logger.error(message)
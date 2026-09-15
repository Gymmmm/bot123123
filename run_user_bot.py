from qiaolian_dual.runtime_guard import (
    acquire_user_bot_polling_lock,
    release_user_bot_polling_lock,
)
from qiaolian_dual.user_bot import main


if __name__ == "__main__":
    lock_handle = acquire_user_bot_polling_lock()
    try:
        main()
    finally:
        release_user_bot_polling_lock(lock_handle)

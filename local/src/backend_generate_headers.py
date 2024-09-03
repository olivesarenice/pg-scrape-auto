import json
import random
import webbrowser
from time import sleep

import pyautogui
from loguru import logger

# HELPERS


def move_cursor_towards_target(target_location):
    try:
        # Get the center coordinates of the target image
        target_center_x = target_location.left + target_location.width / 2
        target_center_y = target_location.top + target_location.height / 2

        # Get the current mouse position
        current_x, current_y = pyautogui.position()

        # Calculate the distance and direction to move towards the target
        distance_x = target_center_x - current_x
        distance_y = target_center_y - current_y

        # Set a step size for movement
        step_size = 10

        # Move the cursor towards the target with slight jiggles
        while abs(distance_x) > step_size or abs(distance_y) > step_size:
            # Calculate a slight jiggle in the movement
            jiggle_x = random.uniform(-2, 2)
            jiggle_y = random.uniform(-2, 2)

            # Move towards the target with the jiggle
            pyautogui.moveRel(
                distance_x + jiggle_x, distance_y + jiggle_y, duration=0.1
            )

            # Update the current mouse position
            current_x, current_y = pyautogui.position()

            # Recalculate the distance to the target
            distance_x = target_center_x - current_x
            distance_y = target_center_y - current_y

            # Add a short delay
            sleep(0.05)
        logger.info(f"Moved cursor to {target_location}")
    except Exception as e:
        logger.error(f"Error: {e}")


def solve_captcha(target_image_path):
    try:
        while True:
            # Locate the target image on the screen
            logger.info(f"Try finding CAPTCHA box ...")
            target_location = pyautogui.locateOnScreen(target_image_path)

            # If the target image is found
            if target_location:
                # Move the cursor towards the target with slight jiggles
                move_cursor_towards_target(target_location)

                # Get the center coordinates of the target image
                center_x = target_location.left + target_location.width / 2
                center_y = target_location.top + target_location.height / 2

                # Click on the center of the target image
                pyautogui.click(center_x, center_y)
                logger.info("Clicked on CAPTCHA!")
                # Break out of the loop once the click is performed
                break
            else:
                # If the target image is not found, wait for a short duration
                logger.warning("Not found, re-trying...")
                sleep(1)
    except Exception as e:
        print(f"Error: {e}")


# SUBROUTINES


def main(cmd_arg, config):

    wait_multiple = config["pyautogui"]["wait_multiple"]
    # Load webpage
    print("Starting browser")
    webbrowser.get(f"{config["path_to_chrome"]} %s --incognito").open(
        config["scraper"]["bot_trigger_url"]
    )
    print("Pause 10 seconds")
    sleep(10)  # Wait until captcha is done
    print("continue")
    solve_captcha(config["pyautogui"]["img_captcha_box"])  # Click on captcha naturally

    sleep(10)  # Wait for captcha to verify

    # Open Inspect
    pyautogui.hotkey("ctrl", "shift", "i")
    sleep(2)

    # Switch back to webpage
    pyautogui.hotkey("alt", "tab")
    sleep(2)

    # Reload page
    pyautogui.hotkey("ctrl", "l")
    sleep(2)

    pyautogui.press("enter")
    sleep(2)

    # Switch to Inspect
    pyautogui.hotkey("alt", "tab")
    sleep(5)

    logger.info("Exporting HAR...")
    # # Click on network tab, if not already open.
    # network_location = pyautogui.locateOnScreen(config["pyautogui"]["img_network"])

    # if network_location:
    #     pyautogui.click(
    #         network_location.left + network_location.width / 2,
    #         network_location.top + network_location.height / 2,
    #     )
    #     sleep(2)
    # else:
    #     logger.error("Network tab not found.")
    #     return False

    # Export HAR
    export_location = pyautogui.locateOnScreen(config["pyautogui"]["img_export"])
    if export_location:
        pyautogui.click(
            export_location.left + export_location.width / 2,
            export_location.top + export_location.height / 2,
        )
        sleep(5)

    else:
        logger.error("Export button not found.")
        return False

    # Do the export.
    pyautogui.press("enter")
    sleep(1)
    pyautogui.press("tab")
    sleep(1)
    pyautogui.press("enter")
    sleep(3)

    # Close both tabs
    pyautogui.hotkey("ctrl", "w")
    pyautogui.hotkey("ctrl", "w")
    sleep(2)

    return True

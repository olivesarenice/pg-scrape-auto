import webbrowser
import pyautogui
import random
from time import sleep
import json

with open("config.json") as config_file:
    config_data = json.load(config_file)

# Function to wait for a short duration
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
            pyautogui.moveRel(distance_x + jiggle_x, distance_y + jiggle_y, duration=0.1)
            
            # Update the current mouse position
            current_x, current_y = pyautogui.position()
            
            # Recalculate the distance to the target
            distance_x = target_center_x - current_x
            distance_y = target_center_y - current_y
            
            # Add a short delay
            sleep(0.05)
    except Exception as e:
        print(f"Error: {e}")

def solve_captcha(target_image_path):
    try:
        while True:
            # Locate the target image on the screen
            print(f'trying to locate {target_image_path}')
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
                
                # Break out of the loop once the click is performed
                break
            else:
                # If the target image is not found, wait for a short duration
                sleep(1)
    except Exception as e:
        print(f"Error: {e}")

def main(path_to_chrome):

    # Load webpage
    print('Starting browser...')
    webbrowser.get(path_to_chrome).open("https://www.propertyguru.com.sg/property-for-sale/20?")
    sleep(3) # Wait until captcha is done
    print('Trying to solve captcha...')
    solve_captcha(config_data['path_to_imgs']['captcha_box']) # Click on captcha naturally
    sleep(5) # Wait for captcha to verify


    # Open Inspect
    pyautogui.hotkey('ctrl', 'shift', 'i')
    sleep(2)

    # Switch back to webpage
    pyautogui.hotkey('alt', 'tab')
    sleep(2)

    # Reload page
    pyautogui.hotkey('ctrl', 'l')
    sleep(2)
    pyautogui.press('enter')
    sleep(2)

    # Switch to Inspect
    pyautogui.hotkey('alt', 'tab')
    sleep(2)

    # Click on network tab, if not already open.
    try:
        network_location = pyautogui.locateOnScreen(config_data['path_to_imgs']['network_unfocus'])
    except:
        print('Network tab is in focus')

    try:
        network_location = pyautogui.locateOnScreen(config_data['path_to_imgs']['network_focus'])
    except:
        print('Network tab is not in focus')

    if network_location:
        pyautogui.click(network_location.left + network_location.width / 2, network_location.top + network_location.height / 2)
        sleep(2)   
    else:
        print("Could not find any Network tabs.")
        return False

    # Export HAR
    exp_har_location = pyautogui.locateOnScreen(config_data['path_to_imgs']['export_har'])
    if exp_har_location:
        pyautogui.click(exp_har_location.left + exp_har_location.width / 2, exp_har_location.top + exp_har_location.height / 2)
        sleep(2)

    else:
        print("Could not find export button")
        return False

    # Do the export.
    pyautogui.press('enter')
    sleep(1)
    pyautogui.press('tab')
    sleep(1)
    pyautogui.press('enter')
    sleep(3)

    # Close both tabs
    pyautogui.hotkey('ctrl', 'w')
    pyautogui.hotkey('ctrl', 'w')
    sleep(2)

    return True

if __name__ == '__main__':

    path_to_chrome = f'{config_data["path_to_chrome"]} %s --incognito'
    main(path_to_chrome)
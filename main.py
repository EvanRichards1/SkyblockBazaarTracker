from time import sleep

from src.bazaar import BazaarTracker

bzt = BazaarTracker("https://api.hypixel.net")
bzt.update()

bzt.update()

bzt.save("./dump")
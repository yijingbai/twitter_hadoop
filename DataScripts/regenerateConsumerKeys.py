from selenium import webdriver
from selenium.webdriver.common.keys import Keys
import time

driver = webdriver.Chrome()


driver.get("https://twitter.com/login")

time.sleep(5)
assert "Login on Twitter" in driver.title

elem = driver.find_element_by_xpath("//*[@id=\"page-container\"]/div/div[1]/form/fieldset/div[1]/input")
elem.send_keys("amangpt777@gmail.com")

elem = driver.find_element_by_xpath("//*[@id=\"page-container\"]/div/div[1]/form/fieldset/div[2]/input")
elem.send_keys("@CounterStrike")
elem.send_keys(Keys.RETURN)

driver.get("https://apps.twitter.com/app/8945531/keys")

assert "HadoopExtract1" in driver.title


elem = driver.find_element_by_xpath("//*[@id=\"gaz-content-body\"]/div[2]/ul/li[3]/a").click()

elem = driver.find_element_by_xpath("//*[@id=\"edit-app-actions\"]/div/a[1]").click()
elem = driver.find_element_by_xpath("//*[@id=\"edit-submit-api-keys\"]").click()

time.sleep(5)
elem = driver.find_element_by_xpath("//*[@id=\"gaz-content-body\"]/div[3]/div/div[2]/div[1]/span[2]")
consumerKey = elem.text
elem = driver.find_element_by_xpath("//*[@id=\"gaz-content-body\"]/div[3]/div/div[2]/div[2]/span[2]")
consumerSecret = elem.text

print consumerKey
print consumerSecret

driver.close()


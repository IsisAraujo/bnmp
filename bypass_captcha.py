from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import NoSuchElementException
import time

class ReCaptchaBypasser:
    def __init__(self, chrome_driver_path, chrome_extension_path):
        self.chrome_driver_path = chrome_driver_path
        self.chrome_extension_path = chrome_extension_path
        self.driver = None

    def initialize_driver(self):
        # Configurações do Chrome
        chrome_options = Options()
        chrome_options.add_extension(self.chrome_extension_path)

        # Criar um objeto Service com o caminho do ChromeDriver
        service = Service(self.chrome_driver_path)

        # Inicializar o driver com o objeto Service
        self.driver = webdriver.Chrome(service=service, options=chrome_options)

    def bypass_recaptcha(self, page_url):
        self.driver.get(page_url)
        time.sleep(2)

        # Trocar para o iframe do reCAPTCHA
        try:
            iframe = self.driver.find_element(By.XPATH, '//iframe[@title="reCAPTCHA"]')
            self.driver.switch_to.frame(iframe)

            # Clicar no checkbox do reCAPTCHA
            checkbox = self.driver.find_element(By.CLASS_NAME, 'recaptcha-checkbox-border')
            checkbox.click()
            self.driver.implicitly_wait(5)
        except NoSuchElementException:
            self.driver.quit()
            exit()

        # Voltar para o conteúdo principal
        self.driver.switch_to.default_content()

        # Trocar para o iframe do desafio do reCAPTCHA
        try:
            challenge_iframe = self.driver.find_element(By.XPATH, '//iframe[@title="recaptcha challenge expires in two minutes"]')
            self.driver.switch_to.frame(challenge_iframe)

            # Clicar no botão da extensão Buster dentro do iframe do reCAPTCHA
            buster_button = self.driver.find_element(By.XPATH, '//*[@id="rc-imageselect"]/div[3]/div[2]/div[1]/div[1]/div[4]')
            buster_button.click()
            time.sleep(4)
        except NoSuchElementException:
            self.driver.quit()
            exit()

        # Voltar para o conteúdo principal
        self.driver.switch_to.default_content()
        time.sleep(8)

    def quit(self):
        # Fechar o navegador
        if self.driver:
            self.driver.quit()

# Exemplo de uso da classe
if __name__ == "__main__":
    chrome_driver_path = "/usr/bin/chromedriver"  # Certifique-se de que o caminho esteja correto
    chrome_extension_path = "buster.crx"  # Caminho para a extensão Buster

    recaptcha_bypasser = ReCaptchaBypasser(chrome_driver_path, chrome_extension_path)
    recaptcha_bypasser.initialize_driver()
    recaptcha_bypasser.bypass_recaptcha("https://portalbnmp.cnj.jus.br/#/captcha/")
    recaptcha_bypasser.quit()

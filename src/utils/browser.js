import { chromium } from 'playwright';

export async function launchBrowser(log) {
  const headless = process.env.HEADLESS !== '0';
  log(`Lançando browser (headless=${headless})`);
  const browser = await chromium.launch({
    headless,
    args: ['--no-sandbox','--disable-dev-shm-usage']
  });
  return browser;
}

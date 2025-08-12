// src/jobs/runner.js
import { chromium } from 'playwright';

export async function runAutomacao({ usuario }) {
  const browser = await chromium.launch({
    headless: true,
    args: ['--no-sandbox','--disable-setuid-sandbox']
  });
  const page = await browser.newPage();
  await page.goto('https://example.com');
  // ... sua lógica (login, scraping, etc.)
  const title = await page.title();
  await browser.close();
  return { ok: true, title, usuario };
}

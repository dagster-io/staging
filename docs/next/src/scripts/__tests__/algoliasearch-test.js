const puppeteer = require('puppeteer');

test('search is live', async () => {
  const browser = await puppeteer.launch();
  const page = await browser.newPage();

  const navigationPromise = page.waitForNavigation();

  await page.goto('https://docs.dagster.io/_apidocs');

  await page.setViewport({ width: 3840, height: 2058 });

  await page.waitForSelector('.flex #search');
  await page.click('.flex #search');

  await page.type('.flex #search', 'ass');

  await page.waitForTimeout(1000);

  await page.waitForSelector(
    '.border-b:nth-child(1) > .block > .px-4 > .flex > .text-sm',
  );
  await page.click(
    '.border-b:nth-child(1) > .block > .px-4 > .flex > .text-sm',
  );

  await navigationPromise;

  await page.waitForSelector('div > #solids > #events > #asset-key > h3');
  await page.click('div > #solids > #events > #asset-key > h3');

  await browser.close();
}, 5000);

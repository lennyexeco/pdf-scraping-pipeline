from flask import Flask, request, jsonify
import asyncio
import logging
import pyppeteer
import os
import json
import atexit
from datetime import datetime

# --- Logging Setup ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(module)s.%(funcName)s:%(lineno)d - %(message)s'
)
logger = logging.getLogger(__name__)

app = Flask(__name__)

class AdvancedRenderer:
    def __init__(self):
        self.browser = None
        self.lock = asyncio.Lock()

    async def init_browser(self):
        """Initialize browser with optimal settings, ensuring only one instance."""
        async with self.lock:
            if not self.browser or self._is_browser_closed():
                if self.browser:
                    logger.warning("Previous browser instance found disconnected or problematic. Attempting to close.")
                    try:
                        await self.browser.close()
                    except Exception as e:
                        logger.warning(f"Error closing previous browser instance: {e}")
                    self.browser = None

                logger.info("Initializing new Pyppeteer browser instance.")
                try:
                    self.browser = await pyppeteer.launch(
                        headless=True,
                        args=[
                            '--no-sandbox',
                            '--disable-setuid-sandbox',
                            '--disable-dev-shm-usage',
                            '--disable-accelerated-2d-canvas',
                            '--disable-gpu',
                            '--window-size=1920x1080',
                            '--disable-extensions',
                            '--disable-plugins-discovery',
                            '--disable-images',  # Keep this for faster loading
                            '--disable-media-source',
                            '--disable-audio-output',
                            '--mute-audio',
                            '--disable-ipc-flooding-protection',
                            '--disable-background-timer-throttling',
                            '--disable-renderer-backgrounding',
                            # Additional args for better Federal Reserve site compatibility
                            '--disable-web-security',
                            '--disable-site-isolation-trials',
                            '--disable-blink-features=AutomationControlled',
                            '--disable-infobars',
                        ],
                    )
                    logger.info(f"Browser initialized. Version: {await self.browser.version()}")
                except Exception as e:
                    logger.error(f"Failed to launch browser: {e}", exc_info=True)
                    self.browser = None
                    raise
            else:
                logger.debug("Reusing existing browser instance.")
        return self.browser

    def _is_browser_closed(self):
        """Check if browser is closed or unusable."""
        if not self.browser:
            return True
        
        try:
            if hasattr(self.browser, 'isConnected'):
                return not self.browser.isConnected()
            elif hasattr(self.browser, '_connection') and self.browser._connection:
                return self.browser._connection.closed
            elif hasattr(self.browser, 'process') and self.browser.process:
                return self.browser.process.poll() is not None
            else:
                return True
        except Exception as e:
            logger.warning(f"Error checking browser status: {e}")
            return True

    async def close_browser(self):
        """Clean up browser resources if it exists."""
        async with self.lock:
            if self.browser and not self._is_browser_closed():
                logger.info("Closing browser instance.")
                try:
                    await self.browser.close()
                except Exception as e:
                    logger.error(f"Error during browser close: {e}")
                self.browser = None
            elif self.browser:
                logger.warning("Browser instance was not connected. Setting to None.")
                self.browser = None

    async def wait_for_page_load_or_change(self, page, timeout_seconds=15, stability_checks=3, check_interval=1.0, previous_content_hash=None):
        """Enhanced wait with better handling for Fed sites that load content dynamically."""
        logger.debug(f"Smart wait started for {page.url} (timeout: {timeout_seconds}s)")
        start_time = datetime.now()
        last_height = 0
        stable_height_count = 0
        
        try:
            # Wait for network idle with longer timeout for Fed sites
            await page.waitForNavigation({'waitUntil': 'networkidle0', 'timeout': 5000})
            logger.debug(f"Network became idle for {page.url}")
        except pyppeteer.errors.TimeoutError:
            logger.debug(f"Network not idle within 5s for {page.url}, continuing with content checks.")
        except Exception as e:
            logger.warning(f"Minor error during networkidle0 check for {page.url}: {e}")

        # Wait for DOM content and any dynamic loading
        try:
            await page.waitForSelector('body', {'timeout': 5000})
            await asyncio.sleep(1)  # Give a moment for any immediate dynamic content
        except Exception as e:
            logger.debug(f"Body selector wait issue: {e}")

        while (datetime.now() - start_time).total_seconds() < timeout_seconds:
            try:
                current_height = await page.evaluate('document.body.scrollHeight')
                
                # Also check for specific Fed site indicators
                fed_content_loaded = await page.evaluate('''
                    () => {
                        // Check for common Fed site content indicators
                        const tables = document.querySelectorAll('table');
                        const links = document.querySelectorAll('a[href*=".pdf"]');
                        const scheduleContent = document.querySelectorAll('[class*="schedule"], [id*="schedule"]');
                        const operationContent = document.querySelectorAll('[class*="operation"], [id*="operation"]');
                        
                        return {
                            hasTable: tables.length > 0,
                            hasPdfLinks: links.length > 0,
                            hasScheduleContent: scheduleContent.length > 0,
                            hasOperationContent: operationContent.length > 0,
                            totalElements: tables.length + links.length + scheduleContent.length + operationContent.length
                        };
                    }
                ''')
                
                logger.debug(f"Fed content check: {fed_content_loaded}")
                
                if current_height == last_height:
                    stable_height_count += 1
                    # If we have good Fed content and stable height, we're probably ready
                    if stable_height_count >= stability_checks and fed_content_loaded['totalElements'] > 0:
                        if previous_content_hash:
                            current_content_hash = hash(await page.content())
                            if current_content_hash != previous_content_hash:
                                stable_height_count = 0
                                previous_content_hash = current_content_hash
                            else:
                                break
                        else:
                            break
                else:
                    stable_height_count = 0
                    last_height = current_height
                    if previous_content_hash:
                        previous_content_hash = hash(await page.content())

            except Exception as e:
                logger.warning(f"Error during content stability check: {e}")
                break

            await asyncio.sleep(check_interval)
        
        elapsed = (datetime.now() - start_time).total_seconds()
        logger.debug(f"Smart wait for {page.url} finished after {elapsed:.2f}s.")
        return previous_content_hash

    async def interact_with_elements(self, page, interactions_config):
        """Enhanced interactions for Federal Reserve sites."""
        performed_interactions = []
        if not interactions_config: 
            return performed_interactions
            
        logger.debug(f"Interacting on {page.url} with config: {interactions_config}")

        if interactions_config.get('activate_tabs', True):
            # Enhanced tab selectors for Fed sites
            tab_selectors = interactions_config.get('tab_selectors_to_activate', [
                'li.ui-tabs-tab:not(.ui-tabs-active) a.ui-tabs-anchor',
                '.tab:not(.active) a', 
                '.nav-tab:not(.active) a',
                '[role="tab"]:not([aria-selected="true"])',
                # Fed-specific selectors
                '.tabs-nav li:not(.active) a',
                '.tabbed-content-nav li:not(.current) a',
                '#tabs li:not(.ui-tabs-active) a'
            ])
            
            script = f"""
                async () => {{
                    let count = 0;
                    const selectors = {json.dumps(tab_selectors)};
                    
                    for (const selector of selectors) {{
                        const elements = document.querySelectorAll(selector);
                        console.log(`Found ${{elements.length}} elements for selector: ${{selector}}`);
                        
                        for (const el of elements) {{
                            const style = window.getComputedStyle(el);
                            const rect = el.getBoundingClientRect();
                            
                            if (style.display !== 'none' && 
                                style.visibility !== 'hidden' && 
                                rect.width > 0 && rect.height > 0) {{
                                
                                try {{
                                    // Scroll element into view
                                    el.scrollIntoView({{ behavior: 'smooth', block: 'center' }});
                                    await new Promise(r => setTimeout(r, 200));
                                    
                                    // Click the element
                                    el.click();
                                    count++;
                                    console.log(`Clicked tab: ${{el.textContent?.trim() || 'Unknown'}}`);
                                    
                                    // Wait for any animations/transitions
                                    await new Promise(r => setTimeout(r, 500));
                                }} catch (e) {{
                                    console.error('Error clicking tab:', e);
                                }}
                            }}
                        }}
                    }}
                    return count;
                }}
            """
            
            try:
                clicked_count = await page.evaluate(script)
                if clicked_count > 0:
                    performed_interactions.append(f"Activated {clicked_count} tab(s).")
                    # Wait longer for Fed site content to load after tab activation
                    await self.wait_for_page_load_or_change(page, timeout_seconds=10)
                else:
                    logger.debug("No tabs found to activate")
            except Exception as e:
                logger.error(f"Error activating tabs on {page.url}: {e}")
                performed_interactions.append(f"Error activating tabs: {str(e)}")
        
        # Handle custom JavaScript
        if interactions_config.get('custom_js'):
            try:
                result = await page.evaluate(interactions_config['custom_js'])
                performed_interactions.append(f"Executed custom_js. Result: {str(result)[:100]}")
                await self.wait_for_page_load_or_change(page, timeout_seconds=interactions_config.get('post_custom_js_wait_s', 5))
            except Exception as e:
                logger.error(f"Error executing custom_js on {page.url}: {e}")
                performed_interactions.append(f"Error in custom_js: {str(e)}")
        
        return performed_interactions

    async def _create_page_and_navigate(self, browser, url, page_load_config):
        """Enhanced page creation with better Fed site compatibility."""
        page = await browser.newPage()
        
        # Set viewport
        await page.setViewport(page_load_config.get('viewport', {'width': 1920, 'height': 1080}))
        
        # Set user agent to appear more like a real browser
        user_agent = page_load_config.get('user_agent', 
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')
        await page.setUserAgent(user_agent)
        
        # Set extra headers for Fed sites
        await page.setExtraHTTPHeaders({
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate',
            'DNT': '1',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        })
        
        # Remove navigator.webdriver property
        await page.evaluateOnNewDocument('''
            () => {
                Object.defineProperty(navigator, 'webdriver', {
                    get: () => undefined,
                });
            }
        ''')
        
        logger.info(f"Navigating new page to: {url}")
        
        try:
            await page.goto(url, {
                'waitUntil': page_load_config.get('wait_until', 'networkidle0'), 
                'timeout': page_load_config.get('timeout_ms', 45000)  # Increased timeout for Fed sites
            })
        except pyppeteer.errors.TimeoutError as e:
            logger.warning(f"Navigation timeout for {url}, but continuing: {e}")
            # Don't fail completely on timeout, the page might still be usable
        
        return page

    async def render_page(self, url, config):
        """Renders a single page with interactions."""
        page = None
        try:
            browser = await self.init_browser()
            if not browser: 
                return {'error': "Browser not initialized", 'url': url}
                
            page = await self._create_page_and_navigate(browser, url, config.get('page_load_config', {}))
            
            # Enhanced wait for Fed sites
            await self.wait_for_page_load_or_change(page, config.get('page_load_config', {}).get('max_wait_initial_s', 15))
            
            # Perform interactions
            interactions_log = await self.interact_with_elements(page, config.get('interactions_config'))
            
            # Get final HTML content
            html_content = await page.content()
            
            # Log some stats about what we found
            pdf_links_count = await page.evaluate('''
                () => document.querySelectorAll('a[href*=".pdf"]').length
            ''')
            
            table_count = await page.evaluate('''
                () => document.querySelectorAll('table').length
            ''')
            
            logger.info(f"Page rendered - PDF links: {pdf_links_count}, Tables: {table_count}")
            
            return {
                'html': html_content, 
                'interactions': interactions_log, 
                'url': page.url, 
                'timestamp': datetime.now().isoformat(),
                'stats': {
                    'pdf_links_found': pdf_links_count,
                    'tables_found': table_count
                }
            }
            
        except Exception as e:
            logger.error(f"render_page failed for {url}: {e}", exc_info=True)
            return {'error': str(e), 'url': url}
        finally:
            if page: 
                await page.close()

    async def render_paginated_table(self, url, pagination_config):
        """Enhanced paginated table rendering."""
        page = None
        collected_html_parts = []
        full_interaction_log = []
        
        try:
            browser = await self.init_browser()
            if not browser: 
                return {"status": "error", "message": "Browser not initialized", 'url': url}
                
            page = await self._create_page_and_navigate(browser, url, pagination_config.get('page_load_config', {}))
            
            # Handle initial interactions
            if initial_interactions_cfg := pagination_config.get('initial_interactions_config'):
                await self.wait_for_page_load_or_change(page, pagination_config.get('page_load_config', {}).get('max_wait_initial_s', 15))
                full_interaction_log.extend(await self.interact_with_elements(page, initial_interactions_cfg))
            
            previous_content_hash = None
            for page_num in range(1, pagination_config.get('max_pages', 10) + 1):
                logger.info(f"Processing page {page_num} for paginated table at {page.url}")
                
                # Enhanced wait for each page
                previous_content_hash = await self.wait_for_page_load_or_change(
                    page, 
                    pagination_config.get('wait_s_per_page', 10), 
                    previous_content_hash=previous_content_hash
                )
                
                # Extract table content
                if current_table_part_html := await page.evaluate(
                    f"(sel) => document.querySelector(sel)?.outerHTML || null", 
                    pagination_config['table_content_selector']
                ):
                    collected_html_parts.append(current_table_part_html)
                else:
                    logger.warning(f"Selector '{pagination_config['table_content_selector']}' not found on page {page_num} of {page.url}.")

                # Check next button status
                next_button_status = await page.evaluate(f"""
                    (sel) => {{
                        const btn = document.querySelector(sel);
                        if (!btn) return {{ found: false }};
                        const style = window.getComputedStyle(btn);
                        const isDisabled = btn.hasAttribute('disabled') || 
                                         btn.classList.contains('disabled') || 
                                         btn.classList.contains('paginationjs-disabled') || 
                                         (btn.closest('li') && (btn.closest('li').classList.contains('disabled') || 
                                          btn.closest('li').classList.contains('paginationjs-disabled')));
                        return {{ 
                            found: true, 
                            disabled: isDisabled, 
                            visible: style.display !== 'none' && style.visibility !== 'hidden' 
                        }};
                    }}
                """, pagination_config['next_page_selector'])
                
                if not next_button_status.get('found') or not next_button_status.get('visible') or next_button_status.get('disabled'):
                    logger.info(f"Next page button condition met for stopping at page {page_num}.")
                    break
                
                if page_num == pagination_config.get('max_pages', 10): 
                    break
                
                # Click next page
                await page.evaluate(f"(sel) => document.querySelector(sel).click()", pagination_config['next_page_selector'])
                full_interaction_log.append(f"Clicked next page ({page_num} -> {page_num + 1})")
                await asyncio.sleep(pagination_config.get('post_click_delay_ms', 500) / 1000.0)  # Slightly longer delay

            return {
                'status': 'success', 
                'url': url, 
                'pages_processed': len(collected_html_parts), 
                'table_pages_html': collected_html_parts, 
                'interactions': full_interaction_log
            }
            
        except Exception as e:
            logger.error(f"render_paginated_table for {url} failed: {e}", exc_info=True)
            return {'status': 'error', 'message': str(e), 'url': url}
        finally:
            if page: 
                await page.close()

# --- Flask Endpoints ---
renderer_instance = AdvancedRenderer()

@app.route('/render', methods=['POST'])
def render_endpoint_sync():
    data = request.get_json()
    if not data or 'url' not in data:
        return jsonify({"error": "URL is required"}), 400
    return jsonify(asyncio.run(renderer_instance.render_page(data['url'], data)))

@app.route('/render_paginated_sync', methods=['POST'])
def render_paginated_sync_endpoint():
    data = request.get_json()
    if not data or 'url' not in data or 'next_page_selector' not in data or 'table_content_selector' not in data:
        return jsonify({"error": "url, next_page_selector, and table_content_selector are required"}), 400
    return jsonify(asyncio.run(renderer_instance.render_paginated_table(data['url'], data)))

@app.route('/health', methods=['GET'])
def health_check():
    return jsonify({"status": "healthy", "timestamp": datetime.now().isoformat()})

# --- Application Lifecycle ---
@atexit.register
def shutdown_signal_handler():
    try:
        logger.info("atexit: Attempting async cleanup.")
        asyncio.run(renderer_instance.close_browser())
    except Exception as e:
        logger.error(f"atexit: Error during shutdown: {e}")

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8080))
    logger.info(f"Starting Flask app on port {port}")
    app.run(host='0.0.0.0', port=port, debug=False)
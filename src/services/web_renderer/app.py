from flask import Flask, request, jsonify
import asyncio
import logging
import pyppeteer
import os
import json
from datetime import datetime

app = Flask(__name__)
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class AdvancedRenderer:
    def __init__(self):
        self.browser = None
        
    async def init_browser(self):
        """Initialize browser with optimal settings"""
        if not self.browser:
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
                    '--disable-plugins',
                    '--disable-images',  # Faster loading
                    '--disable-javascript-harmony-shipping',
                    '--disable-background-timer-throttling',
                    '--disable-renderer-backgrounding',
                    '--disable-backgrounding-occluded-windows',
                    '--disable-client-side-phishing-detection',
                    '--disable-sync',
                    '--disable-translate',
                    '--hide-scrollbars',
                    '--mute-audio'
                ]
            )
        return self.browser

    async def close_browser(self):
        """Clean up browser resources"""
        if self.browser:
            await self.browser.close()
            self.browser = None

    async def wait_for_dynamic_content(self, page, max_wait=10):
        """Smart waiting for dynamic content to load"""
        start_time = datetime.now()
        last_height = 0
        stable_count = 0
        
        while (datetime.now() - start_time).seconds < max_wait:
            # Check if page height has stabilized
            current_height = await page.evaluate('document.body.scrollHeight')
            
            if current_height == last_height:
                stable_count += 1
                if stable_count >= 3:  # Height stable for 3 checks
                    break
            else:
                stable_count = 0
                last_height = current_height
            
            await asyncio.sleep(0.5)

    async def interact_with_elements(self, page, config):
        """Comprehensive element interaction based on configuration"""
        interactions = []
        
        try:
            # Expand accordions and collapsible content
            if config.get('expand_accordions', True):
                accordion_result = await page.evaluate("""
                    () => {
                        let count = 0;
                        // Standard accordion patterns
                        const selectors = [
                            'button[aria-expanded="false"]',
                            '[data-toggle="collapse"]',
                            '.accordion-button.collapsed',
                            '.collapsible:not(.active)',
                            'details:not([open])',
                            '[role="button"][aria-expanded="false"]',
                            '.expand-btn, .toggle-btn',
                            '[data-bs-toggle="collapse"]',
                            '[data-target]:not(.active)',
                            '.dropdown-toggle:not(.show)'
                        ];
                        
                        selectors.forEach(selector => {
                            document.querySelectorAll(selector).forEach(el => {
                                try {
                                    if (el.tagName === 'DETAILS') {
                                        el.open = true;
                                    } else {
                                        el.click();
                                    }
                                    count++;
                                } catch (e) {
                                    console.log('Failed to click:', selector, e);
                                }
                            });
                        });
                        
                        return count;
                    }
                """)
                interactions.append(f"Expanded {accordion_result} accordion/collapsible elements")

            # Handle tabs and tab panels
            if config.get('activate_tabs', True):
                tab_result = await page.evaluate("""
                    () => {
                        let count = 0;
                        const tabSelectors = [
                            '.tab:not(.active)',
                            '.nav-tab:not(.active)',
                            '[role="tab"]:not([aria-selected="true"])',
                            '.tab-button:not(.selected)',
                            '[data-toggle="tab"]',
                            '.ui-tabs-tab:not(.ui-tabs-active)'
                        ];
                        
                        tabSelectors.forEach(selector => {
                            document.querySelectorAll(selector).forEach(tab => {
                                try {
                                    tab.click();
                                    count++;
                                } catch (e) {
                                    console.log('Failed to activate tab:', e);
                                }
                            });
                        });
                        
                        return count;
                    }
                """)
                interactions.append(f"Activated {tab_result} tabs")

            # Trigger lazy loading
            if config.get('trigger_lazy_loading', True):
                lazy_result = await page.evaluate("""
                    () => {
                        let count = 0;
                        // Scroll to trigger lazy loading
                        const lazyElements = document.querySelectorAll(
                            '[data-src], [loading="lazy"], .lazy, .lazyload, [data-lazy]'
                        );
                        
                        lazyElements.forEach(el => {
                            el.scrollIntoView({ behavior: 'instant', block: 'center' });
                            count++;
                        });
                        
                        // Trigger scroll events
                        window.dispatchEvent(new Event('scroll'));
                        window.dispatchEvent(new Event('resize'));
                        
                        return count;
                    }
                """)
                interactions.append(f"Triggered lazy loading for {lazy_result} elements")

            # Handle modals and popups (but don't actually open them, just prepare)
            if config.get('prepare_modals', True):
                modal_result = await page.evaluate("""
                    () => {
                        let count = 0;
                        // Find modal triggers and add visibility styles
                        const modalSelectors = [
                            '[data-toggle="modal"]',
                            '[data-bs-toggle="modal"]',
                            '.modal-trigger',
                            '[href*="#modal"]'
                        ];
                        
                        modalSelectors.forEach(selector => {
                            document.querySelectorAll(selector).forEach(trigger => {
                                const targetId = trigger.getAttribute('data-target') || 
                                               trigger.getAttribute('data-bs-target') ||
                                               trigger.getAttribute('href');
                                
                                if (targetId && targetId.startsWith('#')) {
                                    const modal = document.querySelector(targetId);
                                    if (modal) {
                                        modal.style.display = 'block';
                                        modal.style.visibility = 'visible';
                                        modal.style.opacity = '1';
                                        count++;
                                    }
                                }
                            });
                        });
                        
                        return count;
                    }
                """)
                interactions.append(f"Prepared {modal_result} modals for visibility")

            # Expand select dropdowns (show options)
            if config.get('expand_selects', False):  # Default false as it can be disruptive
                select_result = await page.evaluate("""
                    () => {
                        let count = 0;
                        document.querySelectorAll('select').forEach(select => {
                            try {
                                // Make options visible by temporarily expanding
                                select.setAttribute('size', Math.min(select.options.length, 10));
                                count++;
                            } catch (e) {
                                console.log('Failed to expand select:', e);
                            }
                        });
                        return count;
                    }
                """)
                interactions.append(f"Expanded {select_result} select dropdowns")

            # Trigger hover states for menus
            if config.get('trigger_hover_menus', True):
                hover_result = await page.evaluate("""
                    () => {
                        let count = 0;
                        const hoverSelectors = [
                            '.dropdown:not(.show)',
                            '.menu-item-has-children',
                            '[data-hover="dropdown"]',
                            '.nav-item.dropdown'
                        ];
                        
                        hoverSelectors.forEach(selector => {
                            document.querySelectorAll(selector).forEach(el => {
                                try {
                                    // Simulate hover
                                    el.classList.add('hover', 'show', 'active');
                                    const submenu = el.querySelector('.dropdown-menu, .sub-menu, .submenu');
                                    if (submenu) {
                                        submenu.style.display = 'block';
                                        submenu.style.visibility = 'visible';
                                        submenu.style.opacity = '1';
                                    }
                                    count++;
                                } catch (e) {
                                    console.log('Failed to trigger hover:', e);
                                }
                            });
                        });
                        return count;
                    }
                """)
                interactions.append(f"Triggered hover states for {hover_result} menu elements")

            # Execute custom JavaScript if provided
            if config.get('custom_js'):
                try:
                    custom_result = await page.evaluate(config['custom_js'])
                    interactions.append(f"Executed custom JavaScript: {custom_result}")
                except Exception as e:
                    interactions.append(f"Custom JavaScript failed: {str(e)}")

        except Exception as e:
            logger.error(f"Error during element interaction: {str(e)}")
            interactions.append(f"Error: {str(e)}")

        return interactions

    async def render_page(self, url, config):
        """Main page rendering function with advanced interactions"""
        page = None
        try:
            browser = await self.init_browser()
            page = await browser.newPage()
            
            # Set viewport and user agent
            await page.setViewport({
                'width': config.get('viewport_width', 1920),
                'height': config.get('viewport_height', 1080)
            })
            
            if config.get('user_agent'):
                await page.setUserAgent(config['user_agent'])
            
            # Navigate to page
            logger.info(f"Navigating to: {url}")
            await page.goto(url, {
                'waitUntil': config.get('wait_until', 'networkidle0'),
                'timeout': config.get('timeout', 30000)
            })
            
            # Wait for initial content
            await self.wait_for_dynamic_content(page, config.get('max_wait', 10))
            
            # Perform interactions
            interactions = await self.interact_with_elements(page, config)
            
            # Wait again after interactions
            await asyncio.sleep(config.get('post_interaction_wait', 2))
            await self.wait_for_dynamic_content(page, 5)
            
            # Get final HTML
            html = await page.content()
            
            # Get page metrics
            metrics = await page.metrics()
            
            return {
                'html': html,
                'interactions': interactions,
                'metrics': metrics,
                'url': url,
                'timestamp': datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Failed to render {url}: {str(e)}")
            return None
        finally:
            if page:
                await page.close()

# Global renderer instance
renderer = AdvancedRenderer()

@app.route('/render', methods=['POST'])
def render_html():
    """Enhanced render endpoint with comprehensive configuration"""
    try:
        data = request.get_json()
        url = data.get('url')
        
        if not url:
            return jsonify({"error": "URL is required"}), 400
        
        # Configuration with sensible defaults
        config = {
            'timeout': data.get('timeout', 30000),
            'viewport_width': data.get('viewport_width', 1920),
            'viewport_height': data.get('viewport_height', 1080),
            'wait_until': data.get('wait_until', 'networkidle0'),
            'max_wait': data.get('max_wait', 10),
            'post_interaction_wait': data.get('post_interaction_wait', 2),
            'user_agent': data.get('user_agent'),
            
            # Interaction options
            'expand_accordions': data.get('expand_accordions', True),
            'activate_tabs': data.get('activate_tabs', True),
            'trigger_lazy_loading': data.get('trigger_lazy_loading', True),
            'prepare_modals': data.get('prepare_modals', True),
            'expand_selects': data.get('expand_selects', False),
            'trigger_hover_menus': data.get('trigger_hover_menus', True),
            'custom_js': data.get('custom_js')
        }
        
        # Render page
        result = asyncio.run(renderer.render_page(url, config))
        
        if result:
            # Return different response formats based on request
            if data.get('include_metadata', False):
                return jsonify(result)
            else:
                return jsonify({"html": result['html']})
        else:
            return jsonify({"error": "Failed to render HTML"}), 500
            
    except Exception as e:
        logger.error(f"Error processing request: {str(e)}")
        return jsonify({"error": str(e)}), 500

@app.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    return jsonify({"status": "healthy", "timestamp": datetime.now().isoformat()})

@app.route('/config', methods=['GET'])
def get_config_info():
    """Return available configuration options"""
    config_info = {
        "basic_options": {
            "timeout": "Request timeout in milliseconds (default: 30000)",
            "viewport_width": "Browser viewport width (default: 1920)",
            "viewport_height": "Browser viewport height (default: 1080)",
            "wait_until": "Wait condition: networkidle0, networkidle2, load, domcontentloaded",
            "max_wait": "Maximum wait time for dynamic content (seconds)",
            "user_agent": "Custom user agent string"
        },
        "interaction_options": {
            "expand_accordions": "Expand accordion/collapsible content (default: true)",
            "activate_tabs": "Click on inactive tabs (default: true)",
            "trigger_lazy_loading": "Trigger lazy loading elements (default: true)",
            "prepare_modals": "Make modal content visible (default: true)",
            "expand_selects": "Show select dropdown options (default: false)",
            "trigger_hover_menus": "Show hover menus (default: true)",
            "custom_js": "Custom JavaScript code to execute"
        },
        "response_options": {
            "include_metadata": "Include interaction details and metrics (default: false)"
        }
    }
    return jsonify(config_info)

# Cleanup on shutdown
import atexit

@atexit.register
def cleanup():
    try:
        asyncio.run(renderer.close_browser())
    except:
        pass

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8080))
    app.run(host='0.0.0.0', port=port, debug=False)
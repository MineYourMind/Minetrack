**2.0.0** *(Feb 1st 2016)*
- Servers are now referenced by their name on the graph controls instead of their IP.
- Servers now display their name on hover instead of their IP.
- Graph controls are now saved and loaded automatically.
- Moved server configuration into servers.json from config.json.

**2.1.0** *(Feb 23rd 2016)*
- You can now categorize servers. Add a "category tag" to their entry in ```servers.json```.
- Define the tags in ```config.json```, such as below:

```
"serverCategories": {
	"major": "Major Networks",
	"midsized": "Midsized Networks",
	"small": "Small Networks"
}
```
- If you have no categories, it will create a (hidden) category named "default".
- You can control whether or not categories are visible by default using the "categoriesVisible" tag in ```config.json```. 
  - If true and there's >1 category, the browser will have an option to hide/show the categories. Otherwise the controls are always hidden.
- New endpoint (```publicConfig.json```) allows the browser to know system details before the socket connection is established.
- New header design to make it less annoying.
- Various bug fixes.
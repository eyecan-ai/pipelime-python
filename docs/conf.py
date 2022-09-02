# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

project = "🍋 Pipelime"
copyright = "2022, Eyecan.ai https://www.eyecan.ai/"
author = "Eyecan.ai"

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = [
    "myst_parser",
    "sphinxcontrib.mermaid",
]

templates_path = ["_templates"]
exclude_patterns = ["_build", "Thumbs.db", ".DS_Store"]


# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

html_short_title = "Pipelime Documentation"
html_theme = "sphinx_rtd_theme"
# html_static_path = ["_static"]
# html_logo = "images/logo.png"
# html_favicon = "images/favicon.ico"

html_theme_options = {
    "sticky_navigation": False,
    "prev_next_buttons_location": "both",
    "style_external_links": True,
    "navigation_depth": -1,
    "titles_only": False,
    # "style_nav_header_background": "#ace600",
}

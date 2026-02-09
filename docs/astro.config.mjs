import { defineConfig } from 'astro/config';
import starlight from '@astrojs/starlight';
import rehypeExternalLinks from 'rehype-external-links';

export default defineConfig({
	markdown: {
		rehypePlugins: [
			[rehypeExternalLinks, { target: '_blank', rel: ['noopener', 'noreferrer'] }]
		],
	},
	site: 'https://opencitations.github.io',
	base: '/time-agnostic-library',

	integrations: [
		starlight({
			title: 'time-agnostic-library',
			description: 'A Python library for performing time-travel queries on RDF datasets compliant with the OCDM provenance specification',

			social: [
				{ icon: 'github', label: 'GitHub', href: 'https://github.com/opencitations/time-agnostic-library' },
			],

			sidebar: [
				{
					label: 'Guides',
					items: [
						{ label: 'Getting started', slug: 'getting_started' },
						{ label: 'Version materialization', slug: 'version_materialization' },
						{ label: 'Structured queries', slug: 'structured_queries' },
						{ label: 'Delta queries', slug: 'delta_queries' },
						{ label: 'OCDM conversion', slug: 'ocdm_conversion' },
						{ label: 'Configuration', slug: 'configuration' },
						{ label: 'Contributing', slug: 'contributing' },
					],
				},
			],
		}),
	],
});

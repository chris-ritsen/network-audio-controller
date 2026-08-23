(() => {
	const navigationElement = document.querySelector(".page-contents");
	if (!navigationElement) {
		return;
	}

	const navigationEntries = Array.from(navigationElement.querySelectorAll('a[href^="#"]'))
		.map((linkElement) => {
			const identifier = decodeURIComponent(linkElement.hash.slice(1));
			const sectionElement = document.getElementById(identifier);
			return sectionElement ? { linkElement, sectionElement } : null;
		})
		.filter((entry) => entry !== null);

	if (navigationEntries.length === 0) {
		return;
	}

	let updateScheduled = false;

	const updateCurrentSection = () => {
		const readingLine = Math.min(window.innerHeight * 0.3, 240);
		let currentEntry = navigationEntries[0];

		for (const navigationEntry of navigationEntries) {
			if (navigationEntry.sectionElement.getBoundingClientRect().top > readingLine) {
				break;
			}
			currentEntry = navigationEntry;
		}

		if (Math.ceil(window.scrollY + window.innerHeight) >= document.documentElement.scrollHeight) {
			currentEntry = navigationEntries[navigationEntries.length - 1];
		}

		for (const navigationEntry of navigationEntries) {
			if (navigationEntry === currentEntry) {
				navigationEntry.linkElement.setAttribute("aria-current", "location");
			} else {
				navigationEntry.linkElement.removeAttribute("aria-current");
			}
		}
	};

	const scheduleCurrentSectionUpdate = () => {
		if (updateScheduled) {
			return;
		}
		updateScheduled = true;
		window.requestAnimationFrame(() => {
			updateScheduled = false;
			updateCurrentSection();
		});
	};

	window.addEventListener("scroll", scheduleCurrentSectionUpdate, { passive: true });
	window.addEventListener("resize", scheduleCurrentSectionUpdate);
	window.addEventListener("hashchange", scheduleCurrentSectionUpdate);
	window.addEventListener("load", scheduleCurrentSectionUpdate, { once: true });
	updateCurrentSection();
})();

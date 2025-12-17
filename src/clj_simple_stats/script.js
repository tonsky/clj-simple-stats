function findGroup(e, graphOuter, graphHover) {
  graphOuter = graphOuter || e.currentTarget.closest('.graph_outer');
  graphHover = graphHover || graphOuter.querySelector('.graph_hover');
  // const bb = graphOuter.querySelector('.graph').getBoundingClientRect();
  const el = e.target; // document.elementFromPoint(e.clientX, bb.top + 109);
  if (el.parentNode.tagName === 'g') {
    return el.parentNode;
  }
}

function onGraphMouseMove(e) {
  const graphOuter = e.currentTarget.closest('.graph_outer');
  const graphHover = graphOuter.querySelector('.graph_hover');
  const g = findGroup(e, graphOuter, graphHover);

  if (g) {
    const graphScroll = graphOuter.querySelector('.graph_scroll');

    const value = g.getAttribute('data-v');
    const date = g.getAttribute('data-d');
    if (value && date) {
      const dateObj = new Date(date);
      const formattedDate = dateObj.toLocaleDateString('en-US', { month: 'short', day: 'numeric' });
      graphHover.style.left = (g.querySelector('rect').getAttribute('x') - graphScroll.scrollLeft + 10) + 'px';
      graphHover.style.display = 'block';
      graphHover.textContent = formattedDate + ': ' + value;
    }
  } else {
    graphHover.style.display = 'none';
  }
}

function onGraphMouseLeave(e) {
  const graphOuter = e.currentTarget.closest('.graph_outer');
  const graphHover = graphOuter.querySelector('.graph_hover');
  graphHover.style.display = 'none';
}

function onGraphClick(e) {
  const g = findGroup(e);
  if (g) {
    const date = g.getAttribute('data-d');
    const url = new URL(window.location.href);
    url.searchParams.set('from', date);
    url.searchParams.set('to', date);
    window.location.href = url.toString();
  }
}

function onLoad() {
  const scrollables = document.querySelectorAll('.graph_scroll');
  
  scrollables.forEach((el) => {
    el.scrollLeft = el.scrollWidth;
  });
  
  scrollables.forEach((el) => {
    el.addEventListener('scroll', () => {
      const scrollLeft = el.scrollLeft;
      scrollables.forEach((other) => {
        if (other !== el) {
          other.scrollLeft = scrollLeft;
        }
      });
    });
  });
  
  const graphs = document.querySelectorAll('.graph');
  
  graphs.forEach((graph) => {
    graph.addEventListener('mousemove', onGraphMouseMove);
    graph.addEventListener('mouseleave', onGraphMouseLeave);
    graph.addEventListener('click', onGraphClick);
  });
};

window.addEventListener('load', onLoad);

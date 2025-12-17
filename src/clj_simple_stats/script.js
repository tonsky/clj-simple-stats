function onMouseMove(e) {
  const graphOuter = e.currentTarget.closest('.graph_outer');
  const graphHover = graphOuter.querySelector('.graph_hover');

  // const bb = graphOuter.querySelector('.graph').getBoundingClientRect();
  const el = e.target; // document.elementFromPoint(e.clientX, bb.top + 109);

  if (el.parentNode.tagName === 'g') {
    const g = el.parentNode;
    const graphScroll = graphOuter.querySelector('.graph_scroll');

    const value = g.getAttribute('data-v');
    const date = g.getAttribute('data-d');
    if (value && date) {
      graphHover.style.left = (g.querySelector('rect').getAttribute('x') - graphScroll.scrollLeft + 30) + 'px';
      graphHover.style.display = 'block';
      graphHover.textContent = date + ': ' + value;
    }
  } else {
    graphHover.style.display = 'none';
  }
}

function onMouseLeave(e) {
  const graphOuter = e.currentTarget.closest('.graph_outer');
  const graphHover = graphOuter.querySelector('.graph_hover');
  graphHover.style.display = 'none';
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
    graph.addEventListener('mousemove', onMouseMove);
    graph.addEventListener('mouseleave', onMouseLeave);
  });
};

window.addEventListener('load', onLoad);

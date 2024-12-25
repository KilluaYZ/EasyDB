import 'nprogress/nprogress.css'
import NProgress from 'nprogress'

NProgress.configure({
  easing: 'ease',
  speed: 500,
  showSpinner: true,
  trickleSpeed: 200,
  minimum: 0.3
})

export default NProgress
